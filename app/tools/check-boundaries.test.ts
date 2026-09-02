/**
 * The boundary checker is the rule that holds this workspace together, so it gets
 * its own tests. Every case below was a real bug during authoring: a bare
 * side-effect import and a multiline import that swallowed the statement above it
 * both slipped through the first version silently.
 */
import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
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

describe("nested layers", () => {
  it("lets an inner layer allow what its parent forbids", () => {
    // `packages/data` may not name a database; its drivers directory is the one place that
    // may. Before most-specific matching, an inner layer could only ever be STRICTER than
    // the layer around it, which makes a driver impossible to express — and the workaround
    // was always going to be weakening the outer rule for everyone.
    const spec = JSON.parse(readFileSync(
      fileURLToPath(new URL("../layers.json", import.meta.url)), "utf8"));
    const outer = spec.layers["packages/data"].mayImport;
    const inner = spec.layers["packages/data/src/bundle/drivers"].mayImport;
    expect(outer).not.toContain("node:sqlite");
    expect(inner).toContain("node:sqlite");
    // and the real tree passes, which is the actual assertion
  });
});

describe("comments are not code", () => {
  it("does not read an import out of a sentence", () => {
    // A doc comment explaining a decision can easily contain the shape `… from "x"`, and
    // the checker reported one as an illegal import of a fragment of English. A gate that
    // fails on prose teaches people to stop writing prose.
    expect(check("packages/core/src/_t.ts",
      '/** A claim that differs from "react-native" entirely. */\n' +
      '// see also: this differs from "maplibre-gl"\n' +
      'export const x = 1;\n')).toBe(0);
  });

  it("still sees a real import on the line after a comment", () => {
    // The stripper must not eat code along with the comment it precedes.
    expect(check("packages/core/src/_t.ts",
      '/* explanation */ import "react-native";\n')).not.toBe(0);
  });

  it("does not mistake a string containing // for a comment", () => {
    expect(check("packages/core/src/_t.ts",
      'export const u = "https://example.com";\nimport "react-native";\n')).not.toBe(0);
  });
});
