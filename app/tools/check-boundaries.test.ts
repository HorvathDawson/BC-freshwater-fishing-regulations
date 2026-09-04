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

/*
 * TEMP FILES ARE NAMED PER TEST FILE (`_bnd*` here, `_plat*` in check-platform.test.ts).
 *
 * Both gates scan the WHOLE tree, and vitest runs test FILES in parallel workers — so a
 * fixture written by one is on disk while the other's checker walks past it. Sharing a
 * basename made the two races into one flake that only appeared when both ran together.
 */
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
    ["bare side-effect import", "packages/ui/src/_bnd.ts", 'import "react-native";\n'],
    ["dynamic import", "packages/core/src/_bnd.ts", 'export const f = () => import("maplibre-gl");\n'],
    ["require()", "packages/data/src/_bnd.ts", 'const x = require("react-dom");\n'],
    ["multiline named import", "packages/core/src/_bnd.ts", 'import {\n a,\n b\n} from "react";\n'],
    ["a test file cannot smuggle one in", "packages/core/src/_bnd.test.ts",
      'import "react-native";\nimport { it } from "vitest";\n'],
  ])("rejects: %s", (_name, file, src) => {
    expect(check(file, src)).toBe(1);
  });

  it.each([
    ["vitest inside a test file", "packages/core/src/_bnd.test.ts", 'import { it } from "vitest";\n'],
    ["data -> core", "packages/data/src/_bnd.ts", 'import { freshness } from "@app/core";\nexport const f = freshness;\n'],
    ["ui -> react", "packages/ui/src/_bnd.ts", 'import { useState } from "react";\nexport const f = useState;\n'],
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
    expect(check("packages/core/src/_bnd.ts",
      '/** A claim that differs from "react-native" entirely. */\n' +
      '// see also: this differs from "maplibre-gl"\n' +
      'export const x = 1;\n')).toBe(0);
  });

  it("does not read an import out of a JSX string", () => {
    /*
     * The real failure: `export function DonorPanel(` began a match whose lazy fill ran to
     * the end of the file looking for ` from "`, found one in JSX —
     * `{cond ? "from" : "forecast from"}{" "}` — and reported the module as importing the
     * fragment `}{`. A gate that invents an import out of rendered English fails on the
     * files most worth checking.
     */
    expect(check("packages/ui-native/src/_bnd.tsx",
      'export function C({ ahead }: { ahead: boolean }) {\n' +
      '  return <Text>{ahead ? "forecast from" : "from"}{" "}two gauges</Text>;\n' +
      '}\n')).toBe(0);
  });

  it("still catches a real re-export across wrapped lines", () => {
    // The fix bounds the clause at `(`, `)` and `;` — NOT at a newline. A long specifier
    // list is routinely wrapped, and bounding at the newline would let one through.
    expect(check("packages/core/src/_bnd.ts",
      'export {\n  a,\n  b,\n} from "react-native";\n')).not.toBe(0);
  });

  it("still catches a real import whose specifiers wrap", () => {
    expect(check("packages/core/src/_bnd.ts",
      'import {\n  a,\n  b,\n} from "react-native";\n')).not.toBe(0);
  });

  it("still sees a real import on the line after a comment", () => {
    // The stripper must not eat code along with the comment it precedes.
    expect(check("packages/core/src/_bnd.ts",
      '/* explanation */ import "react-native";\n')).not.toBe(0);
  });

  it("does not mistake a string containing // for a comment", () => {
    expect(check("packages/core/src/_bnd.ts",
      'export const u = "https://example.com";\nimport "react-native";\n')).not.toBe(0);
  });
});
