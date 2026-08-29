/**
 * The boundary checker is the rule that holds this workspace together, so it gets
 * its own tests. Every case below was a real bug during authoring: a bare
 * side-effect import and a multiline import that swallowed the statement above it
 * both slipped through the first version silently.
 */
import { execFileSync } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { describe, expect, it } from "vitest";

const ROOT = new URL("..", import.meta.url).pathname;

function check(file: string, source: string): number {
  const path = `${ROOT}${file}`;
  writeFileSync(path, source);
  try {
    execFileSync("node", [`${ROOT}tools/check-boundaries.mjs`], { stdio: "pipe" });
    return 0;
  } catch (e: any) {
    return e.status ?? 1;
  } finally {
    rmSync(path, { force: true });
  }
}

describe("layer boundaries", () => {
  it("passes on a clean tree", () => {
    expect(execFileSync("node", [`${ROOT}tools/check-boundaries.mjs`]).toString())
      .toContain("clean");
  });

  it.each([
    ["bare side-effect import", "packages/ui/src/_t.ts", 'import "react-native";\n'],
    ["dynamic import", "packages/core/src/_t.ts", 'export const f = () => import("maplibre-gl");\n'],
    ["require()", "packages/data/src/_t.ts", 'const x = require("react-dom");\n'],
    ["multiline named import", "packages/core/src/_t.ts", 'import {\n a,\n b\n} from "react";\n'],
    ["a test file cannot smuggle one in", "packages/core/src/_t.test.ts",
      'import "react-native";\nimport { it } from "vitest";\n'],
  ])("rejects: %s", (_name, file, src) => {
    expect(check(file, src)).toBe(1);
  });

  it.each([
    ["vitest inside a test file", "packages/core/src/_t.test.ts", 'import { it } from "vitest";\n'],
    ["data -> core", "packages/data/src/_t.ts", 'import { freshness } from "@app/core";\nexport const f = freshness;\n'],
    ["ui -> react", "packages/ui/src/_t.ts", 'import { useState } from "react";\nexport const f = useState;\n'],
  ])("allows: %s", (_name, file, src) => {
    expect(check(file, src)).toBe(0);
  });
});
