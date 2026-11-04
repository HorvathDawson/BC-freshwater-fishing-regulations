/**
 * T12: the VM-fixture writer's pure parts — the golden → app sheet key, `freezePage`, `stableJson`. (Writing and checking
 * the frozen pages is harness.parity.test.ts, from T21.)
 */
import { describe, expect, it } from "vitest";
import type { RegsPage } from "@app/core";
import { FIXTURE_DIR, FIXTURE_STATES, freezePage, goldenSheetKeys, stableJson, appSheetKey } from "./fixtures";

describe("appSheetKey: the golden's state-id-bearing keys → the app's", () => {
  it("every kind the golden opens", () => {
    const id = "vedder_515";
    expect(["other:vedder_515|o10", "pos:vedder_515|pos", "src:vedder_515|r2",
            "srcs:bait|vedder_515", "srcs:gl|vedder_515|hook", "gl:bait_ban", "sp:CT", "hatch", "parts",
            "rule:r1:campbell_river@1-10::campbell_river.r1"].map((k) => appSheetKey(k, id)))
      .toEqual(["other:o10", "pos:pos", "src:r2", "srcs:bait", "srcs:gl|hook", "gl:bait_ban", "sp:CT", "hatch",
                "parts", "rule:r1:campbell_river@1-10::campbell_river.r1"]);
  });

  it("no golden key of a fixture state keeps its state id; none is a licsrc: (owner 2026-11-04: licence sources sheet removed)", () => {
    for (const id of FIXTURE_STATES) {
      const keys = goldenSheetKeys(id);
      expect(keys.filter((k) => k.includes(id) || k.startsWith("licsrc:")), id).toEqual([]);
      expect(keys.length, id).toBeGreaterThan(0);
    }
  });
});

describe("freezePage + stableJson", () => {
  const page = { version: "regs-v38", z: 1, a: { y: [2, { d: 1, c: 0 }], b: "x" },
                 sheet: (k: string) => k === "gone" ? null : { key: k, title: `T ${k}` } } as unknown as RegsPage;

  it("drops the sheet function, opens each key once (sorted), sorts every object's keys, 1-space indent", () => {
    expect(stableJson(freezePage(page, ["pos:pos", "gone", "gl:a", "pos:pos"]))).toBe([
      "{", ' "a": {', '  "b": "x",', '  "y": [', "   2,", "   {", '    "c": 0,', '    "d": 1', "   }", "  ]", " },",
      ' "sheets": {', '  "gl:a": {', '   "key": "gl:a",', '   "title": "T gl:a"', "  },", '  "gone": null,',
      '  "pos:pos": {', '   "key": "pos:pos",', '   "title": "T pos:pos"', "  }", " },",
      ' "version": "regs-v38",', ' "z": 1', "}", ""].join("\n"));
  });

  it("refuses what JSON would silently change: Infinity, NaN, a function", () => {
    expect(() => stableJson({ a: [1, Infinity] })).toThrow("stableJson: Infinity at a[1]");
    expect(() => stableJson({ n: { m: NaN } })).toThrow("stableJson: NaN at n.m");
    expect(() => stableJson({ f: () => 1 })).toThrow("stableJson: a function at f");
  });

  it("the fixtures land in ui-native, one per tab-worthy state", () => {
    expect(FIXTURE_DIR.endsWith("/app/packages/ui-native/src/regs/fixtures")).toBe(true);
    expect(FIXTURE_STATES).toEqual(["vedder_1010", "vedder_615", "vedder_515", "klwa_1010", "bc1_501", "glenn_501"]);
  });
});
