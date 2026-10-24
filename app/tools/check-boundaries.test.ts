/**
 * THE TWO GATES THAT SCAN THE WHOLE TREE, tested together in ONE FILE because they cannot
 * be tested apart.
 *
 * Each test writes a fixture into a scratch tree under the OS temp dir (`inScratch`, T156) and runs the real checker
 * over it through `CHECK_ROOT`. They used to write into the real workspace, where other suites walking packages/ in
 * parallel workers met a half-deleted fixture (ENOENT); the scratch trees end that. The file still keeps both gates
 * together, and the fixture names (`_bnd*` / `_plat*`) stay unique so a failure is legible.
 *
 * Every case below was a real bug during authoring: a bare side-effect import, a multiline
 * import that swallowed the statement above it, an import read out of a doc comment, and a
 * banned API read out of the comment explaining why the file does not call it.
 */
import { execFileSync } from "node:child_process";
import { copyFileSync, existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

const ROOT = new URL("..", import.meta.url).pathname;

/*
 * FIXTURES ARE WRITTEN OUTSIDE THE APP TREE (T156).
 *
 * Both gates scan a whole tree, and so do other suites (check-colours, one-speaker, no-localhost) that vitest runs in
 * parallel workers. A fixture written into packages/ was on disk while their walks listed it and gone when they read
 * it: ENOENT on `_plat.web.ts`, a flake that appeared only when they ran together. So each case gets a scratch tree
 * under the OS temp dir — the app's `layers.json` and the fixture, nothing else — and the real gate runs on it through
 * `CHECK_ROOT`. The real tree is checked as is by each gate's "passes on a clean tree".
 */
function inScratch<T>(files: Record<string, string>, run: (root: string) => T): T {
  const root = mkdtempSync(join(tmpdir(), "app-gate-"));
  try {
    copyFileSync(`${ROOT}layers.json`, join(root, "layers.json"));
    for (const [f, src] of Object.entries(files)) {
      mkdirSync(dirname(join(root, f)), { recursive: true });
      writeFileSync(join(root, f), src);
    }
    return run(root);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
}

/** Run one gate over `root`; its exit code and what it said on stderr. */
function runGate(tool: string, root: string): { status: number; stderr: string } {
  try {
    execFileSync("node", [`${ROOT}tools/${tool}`], { stdio: "pipe", env: { ...process.env, CHECK_ROOT: root } });
    return { status: 0, stderr: "" };
  } catch (e: any) {
    return { status: e.status ?? 1, stderr: String(e.stderr ?? "") };
  }
}

/** One gate over a scratch tree holding `files`; its exit code. */
const gate = (tool: string, files: Record<string, string>): number =>
  inScratch(files, (root) => runGate(tool, root).status);

const check = (file: string, source: string): number => gate("check-boundaries.mjs", { [file]: source });

describe("the fixtures (T156)", () => {
  it("are written outside the app tree, where no other suite walks", () => {
    // mutation: `inScratch` making its tree under ROOT -> fails
    const file = "packages/ui-native/src/_plat.web.ts";
    inScratch({ [file]: "export const a = 1;\n" }, (root) => {
      expect(root.startsWith(ROOT)).toBe(false);
      expect(existsSync(join(root, file))).toBe(true);
      expect(existsSync(`${ROOT}${file}`)).toBe(false);
    });
  });

  it("reach the gate: each gate reports a scratch tree's violation under its own path", () => {
    // mutation: a gate ignoring CHECK_ROOT (it walks the clean real tree instead) -> fails
    expect(inScratch({ "packages/core/src/_bnd.ts": 'import "react-native";\n' },
      (root) => runGate("check-boundaries.mjs", root).stderr.split("\n")[0])).toBe("✗ packages/core/src/_bnd.ts");
    expect(inScratch({ "packages/ui-native/src/_plat.ts": "export const el = document.body;\n" },
      (root) => runGate("check-platform.mjs", root).stderr.split("\n")[0])).toBe(
      '✗ packages/ui-native/src/_plat.ts: uses "document." in shared code — DOM is absent in React Native');
  });
});

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
    const inner = spec.layers["packages/data/src/sql/drivers"].mayImport;
    expect(outer).not.toContain("node:sqlite");
    expect(inner).toContain("node:sqlite");
    // and the real tree passes, which is the actual assertion
  });
});

describe("the release drivers (P11)", () => {
  it("may name a platform database and file system; the loader around them may not", () => {
    expect(check("packages/data/src/release/drivers/_bnd.web.ts",
      'import initSqlJs from "sql.js";\nexport const x = initSqlJs;\n')).toBe(0);
    expect(check("packages/data/src/release/drivers/_bnd.native.ts",
      'import { File } from "expo-file-system";\nexport const x = File;\n')).toBe(0);
    expect(check("packages/data/src/release/_bnd.ts",
      'import { openDatabaseAsync } from "expo-sqlite";\nexport const x = openDatabaseAsync;\n')).not.toBe(0);
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

/** The platform gate over a scratch tree holding `files` (see `inScratch`); its exit code. */
const checkPlatform = (files: Record<string, string>): number => gate("check-platform.mjs", files);

describe("platform parity", () => {
  it("passes on a clean tree", () => {
    expect(execFileSync("node", [`${ROOT}tools/check-platform.mjs`]).toString())
      .toContain("clean");
  });

  it("catches a banned API in shared code", () => {
    expect(checkPlatform({ "packages/ui-native/src/_plat.ts":
      'export const el = document.createElement("canvas");\n' })).not.toBe(0);
  });

  it("catches accessibilityState, which react-native-web silently ignores", () => {
    // It does not warn and it does not throw — the prop is dropped and the element renders
    // with no aria attribute at all, so a screen reader sees nothing and every test that
    // looks at the DOM the way a sighted reader looks at the screen still passes.
    expect(checkPlatform({ "packages/ui-native/src/_plat.tsx":
      'export const a = <View accessibilityState={{ selected: true }} />;\n' })).not.toBe(0);
  });

  it("does not read a banned API out of a comment", () => {
    // THE REGRESSION. `hatch.ts` explains that `document.createElement("canvas")` is a
    // browser call this package may not make, and the gate failed on the explanation.
    expect(checkPlatform({ "packages/ui-native/src/_plat.ts":
      '/**\n' +
      ' * Not a canvas: `document.createElement("canvas")` is a browser API this package\n' +
      ' * may not touch, so the pattern is built as bytes instead.\n' +
      ' */\n' +
      '// see also: accessibilityState is banned here\n' +
      'export const x = 1;\n' })).toBe(0);
  });

  it("still sees a real call on the line after a comment", () => {
    // The stripper must not eat code along with the comment it precedes.
    expect(checkPlatform({ "packages/ui-native/src/_plat.ts":
      '/* explanation */ export const el = document.body;\n' })).not.toBe(0);
  });

  it("keeps string literals, where a banned API is usually a real dynamic call", () => {
    expect(checkPlatform({ "packages/ui-native/src/_plat.ts":
      'export const k = "document.";\n' })).not.toBe(0);
  });

  it("ignores platform variant files, which may use their own platform's APIs", () => {
    // Both halves, because an incomplete variant set is its own violation — that is how a
    // feature ships on one platform only.
    expect(checkPlatform({
      "packages/ui-native/src/_plat.web.ts": 'export const el = document.createElement("div");\n',
      "packages/ui-native/src/_plat.native.ts": 'export const el = null;\n',
    })).toBe(0);
  });

  it("catches a variant that ships on one platform only", () => {
    expect(checkPlatform({
      "packages/ui-native/src/_plat.web.ts": 'export const el = 1;\n',
    })).not.toBe(0);
  });

  it("holds @app/data's platform pairs too (the release drivers, P11)", () => {
    // packages/data holds the database and file-system drivers, which are exactly the pairs that ship on one platform
    // only when one half is forgotten. It was outside the variant scan, so a lone `.web.ts` there passed.
    expect(checkPlatform({
      "packages/data/src/release/drivers/_plat.web.ts": 'export const el = 1;\n',
    })).not.toBe(0);
    expect(checkPlatform({
      "packages/data/src/release/drivers/_plat.web.ts": 'export const a = 1;\nexport const b = 2;\n',
      "packages/data/src/release/drivers/_plat.native.ts": 'export const a = 1;\n',
    })).not.toBe(0);
  });

  it("catches variants whose export surfaces differ", () => {
    // A complete set still diverges if one side exports a helper the other does not —
    // callers compile on one platform and fail on the other.
    expect(checkPlatform({
      "packages/ui-native/src/_plat.web.ts": 'export const a = 1;\nexport const b = 2;\n',
      "packages/ui-native/src/_plat.native.ts": 'export const a = 1;\n',
    })).not.toBe(0);
  });
});
