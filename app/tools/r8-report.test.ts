/**
 * (T1009) r8-report.mjs: the build check on R8's merged configuration and on the bundle's own scores (r8.json, what Play
 * reads). A planted -dontobfuscate / -dontoptimize / -dontshrink fails the check, named with its source; a category
 * under its floor fails; a missing category is refused, never read as 0. Mutation-checked: deleting the `-dont*` filter
 * or the floor comparison fails these.
 */
import { spawnSync } from "node:child_process";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { describe, expect, it } from "vitest";
import { FLOORS, PLAY_LINE, belowFloors, broadRules, passesOff, scoreLine, scoresOf, shortSource } from "./r8-report.mjs";

const TOOL = resolve(process.cwd(), "tools/r8-report.mjs");   // vitest runs from app/
const HEAD = (f: string) => `# The proguard configuration file for the following section is ${f}\n`;
const CONFIG = `${HEAD("/x/app/apps/mobile/android/app/proguard-rules.pro")}-keep class a.b.C { <init>(); }\n# -dontobfuscate in a comment\n` +
  `${HEAD("/g/transformed/lib-1.0/proguard.txt")}-keep class lib.** { *; }\n-keepattributes SourceFile\n`;

function run(...args: string[]) {
  return spawnSync("node", [TOOL, ...args], { encoding: "utf8" });
}
function file(text: string) {
  const d = mkdtempSync(join(tmpdir(), "r8-report-"));
  writeFileSync(join(d, "configuration.txt"), text);
  return join(d, "configuration.txt");
}

describe("T1009: no R8 pass is turned off, by us or by a library", () => {
  it("a clean configuration passes (a commented -dontobfuscate is no rule)", () => {
    expect(passesOff(CONFIG)).toEqual([]);
    const r = run("check", "--config", file(CONFIG));
    expect(r.status).toBe(0);
    expect(r.stdout.trim()).toBe("R8 configuration ok (no -dontoptimize / -dontshrink / -dontobfuscate)");
  });

  for (const pass of ["-dontobfuscate", "-dontoptimize", "-dontshrink"]) {
    it(`a planted ${pass} from a library fails the check, its source named`, () => {
      const planted = `${CONFIG}${HEAD("/g/transformed/evil-2.0/proguard.txt")}${pass}\n`;
      expect(passesOff(planted)).toEqual([{ source: "/g/transformed/evil-2.0/proguard.txt", line: pass }]);
      const r = run("check", "--config", file(planted));
      expect(r.status).toBe(1);
      expect(r.stderr).toContain(`r8-report: R8's configuration turns a pass off: ${pass}  (from /g/transformed/evil-2.0/proguard.txt)`);
    });
  }

  it("-dontnote / -dontwarn are not passes", () => {
    expect(passesOff("-dontnote a.**\n-dontwarn b.**\n")).toEqual([]);
  });
});

describe("T1009: the bundle's scores (r8.json) and the floors", () => {
  // R8 9.2.14's own shape (BUNDLE-METADATA/com.android.tools/r8.json of the T1009 build, trimmed)
  const json = { options: { isObfuscationEnabled: true, isOptimizationsEnabled: true, isShrinkingEnabled: true },
                 stats: { noObfuscationPercentage: 35.1, noOptimizationPercentage: 48.8, noShrinkingPercentage: 30 }, version: "9.2.14" };

  it("reads the three scores as 100 minus R8's held-back shares", () => {
    expect(scoresOf(json)).toEqual({ optimization: 51.2, shrinking: 70, obfuscation: 64.9 });
  });

  it("a missing or impossible figure is refused, never read as 0", () => {
    expect(() => scoresOf({ stats: { noOptimizationPercentage: 1, noShrinkingPercentage: 2 } }))
      .toThrow(/r8.json has no obfuscation percentage/);
    expect(() => scoresOf({ stats: { ...json.stats, noShrinkingPercentage: "30" } })).toThrow(/no shrinking percentage/);
    expect(() => scoresOf({ stats: { ...json.stats, noShrinkingPercentage: 130 } })).toThrow(/no shrinking percentage/);
    expect(() => scoresOf({})).toThrow(/no optimization, shrinking, obfuscation percentage/);
  });

  it("a pass R8 ran with off is refused, whatever the figures say", () => {
    expect(() => scoresOf({ ...json, options: { ...json.options, isObfuscationEnabled: false } }))
      .toThrow(/R8 ran with obfuscation OFF/);
  });

  it("a category under its floor is named; at the floor passes", () => {
    const floors = { optimization: 50, shrinking: 60, obfuscation: 65 };
    expect(belowFloors(scoresOf(json), floors)).toEqual([{ kind: "obfuscation", got: 64.9, floor: 65 }]);
    expect(belowFloors({ optimization: 72, shrinking: 72.3, obfuscation: 69.9 })).toEqual([{ kind: "obfuscation", got: 69.9, floor: FLOORS.obfuscation }]);
    expect(belowFloors({ optimization: 50, shrinking: 60, obfuscation: 65 }, floors)).toEqual([]);
  });

  it("the floors are at or over Play's alert line, every category", () => {
    for (const v of Object.values(FLOORS)) expect(v).toBeGreaterThanOrEqual(PLAY_LINE);
  });

  it("one line for the build output and the email", () => {
    expect(scoreLine({ optimization: 51.24, shrinking: 70, obfuscation: 64.9 }, 7_900_000, { optimization: 1, shrinking: 2, obfuscation: 3 }))
      .toBe("R8 optimization 51.2%, shrinking 70%, obfuscation 64.9% (floors 1/2/3; Play's line 25%; DEX 7.9 MB uncompressed)");
  });
});

describe("T1009: the broad keep rules, with what they match", () => {
  it("counts the kept classes and members a package wildcard matches, rules joined over lines", () => {
    const seeds = "lib.A\nlib.A: void f()\nlib.sub.B\nlib.sub.B: int x\nlib.sub.B: void g()\nother.C\n";
    const cfg = `${CONFIG}-keep class lib.sub.** {\n  *;\n}\n-keep class other.C\n`;
    const out = broadRules(cfg, seeds);
    expect(out.map((r) => [r.rule, r.classes, r.members])).toEqual([
      ["-keep class lib.** { *; }", 2, 3],
      ["-keep class lib.sub.** { *; }", 1, 2],
    ]);
    expect(shortSource(out[0].source)).toBe("lib-1.0/proguard.txt");
    expect(shortSource("/x/y/app/apps/mobile/android/app/proguard-rules.pro")).toBe("app/apps/mobile/android/app/proguard-rules.pro");
  });
});
