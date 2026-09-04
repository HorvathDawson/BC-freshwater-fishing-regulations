/**
 * The platform checker bans APIs that exist on one side of react-native-web and not the
 * other. It reads source as text, which means it can be fooled by prose — and it was: the
 * most useful comment a shared file can carry is the one saying WHY it does not reach for
 * the banned API, and that sentence was reported as the violation it exists to prevent.
 */
import { execFileSync } from "node:child_process";
import { rmSync, writeFileSync } from "node:fs";
import { describe, expect, it } from "vitest";

const ROOT = new URL("..", import.meta.url).pathname;

/** Write files into the shared tree, run the checker, return its exit code. */
function check(files: Record<string, string>): number {
  const paths = Object.keys(files).map((f) => `${ROOT}${f}`);
  for (const [f, src] of Object.entries(files)) writeFileSync(`${ROOT}${f}`, src);
  try {
    execFileSync("node", [`${ROOT}tools/check-platform.mjs`], { stdio: "pipe" });
    return 0;
  } catch (e: any) {
    return e.status ?? 1;
  } finally {
    for (const p of paths) rmSync(p, { force: true });
  }
}

describe("platform parity", () => {
  it("passes on a clean tree", () => {
    expect(execFileSync("node", [`${ROOT}tools/check-platform.mjs`]).toString())
      .toContain("clean");
  });

  it("catches a banned API in shared code", () => {
    expect(check({ "packages/ui-native/src/_t.ts":
      'export const el = document.createElement("canvas");\n' })).not.toBe(0);
  });

  it("catches accessibilityState, which react-native-web silently ignores", () => {
    // It does not warn and it does not throw — the prop is dropped and the element renders
    // with no aria attribute at all, so a screen reader sees nothing and every test that
    // looks at the DOM the way a sighted reader looks at the screen still passes.
    expect(check({ "packages/ui-native/src/_t.tsx":
      'export const a = <View accessibilityState={{ selected: true }} />;\n' })).not.toBe(0);
  });

  it("does not read a banned API out of a comment", () => {
    // THE REGRESSION. `hatch.ts` explains that `document.createElement("canvas")` is a
    // browser call this package may not make, and the gate failed on the explanation.
    expect(check({ "packages/ui-native/src/_t.ts":
      '/**\n' +
      ' * Not a canvas: `document.createElement("canvas")` is a browser API this package\n' +
      ' * may not touch, so the pattern is built as bytes instead.\n' +
      ' */\n' +
      '// see also: accessibilityState is banned here\n' +
      'export const x = 1;\n' })).toBe(0);
  });

  it("still sees a real call on the line after a comment", () => {
    // The stripper must not eat code along with the comment it precedes.
    expect(check({ "packages/ui-native/src/_t.ts":
      '/* explanation */ export const el = document.body;\n' })).not.toBe(0);
  });

  it("keeps string literals, where a banned API is usually a real dynamic call", () => {
    expect(check({ "packages/ui-native/src/_t.ts":
      'export const k = "document.";\n' })).not.toBe(0);
  });

  it("ignores platform variant files, which may use their own platform's APIs", () => {
    // Both halves, because an incomplete variant set is its own violation — that is how a
    // feature ships on one platform only.
    expect(check({
      "packages/ui-native/src/_t.web.ts": 'export const el = document.createElement("div");\n',
      "packages/ui-native/src/_t.native.ts": 'export const el = null;\n',
    })).toBe(0);
  });

  it("catches a variant that ships on one platform only", () => {
    expect(check({
      "packages/ui-native/src/_t.web.ts": 'export const el = 1;\n',
    })).not.toBe(0);
  });

  it("catches variants whose export surfaces differ", () => {
    // A complete set still diverges if one side exports a helper the other does not —
    // callers compile on one platform and fail on the other.
    expect(check({
      "packages/ui-native/src/_t.web.ts": 'export const a = 1;\nexport const b = 2;\n',
      "packages/ui-native/src/_t.native.ts": 'export const a = 1;\n',
    })).not.toBe(0);
  });
});
