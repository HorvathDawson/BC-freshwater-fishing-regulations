/**
 * T12: the VM-fixture writer's pure parts — `sheetKeysOf`, `freezePage`, `stableJson`. (Writing and checking
 * the frozen pages is harness.parity.test.ts, from T21.)
 */
import { describe, expect, it } from "vitest";
import type { RegsPage } from "@app/core";
import { FIXTURE_DIR, FIXTURE_STATES, freezePage, sheetKeysOf, stableJson } from "./fixtures";

describe("sheetKeysOf: the keys a page names", () => {
  it("the two always-open sheets (no Parts sheet: T698), plus every sheet key found anywhere in the view model (not under `sheet`)", () => {
    const page = { keep: { rows: [{ open: "pos:pos" }, { open: "gl:bait_ban" }, "not a key"] }, sheet: () => "gl:ignored" } as unknown as RegsPage;
    expect(sheetKeysOf(page).sort()).toEqual(["gl:bait_ban", "hatch", "pos:pos"]);
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
